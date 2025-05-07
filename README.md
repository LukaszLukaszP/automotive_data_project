# Automotive Data Project

This project is an extensible, end-to-end automotive data pipeline built in Python. It is designed to collect, clean, enrich, and store car listing data — currently from [Otomoto.pl](https://otomoto.pl) — in a structured PostgreSQL database. Future plans include advanced analytics, ML/AI modules, dashboard visualizations, and integration with conversational AI.

---

## 🚗 Features

* **Web scraping** with anti-duplicate logic
* **Dynamic listing segmentation** to avoid pagination limits
* **Data cleaning & transformation** including units, types, and missing values
* **Equipment parsing & normalization**
* **PostgreSQL schema creation and data loading**
* **Modular architecture** ready for additional sources and ML/AI features

---

## 🗂 Project Structure

```text
.
├── data/                  # CSV exports, IDs, model mappings
├── notebooks/            # Jupyter Notebooks (EDA, cleaning, legacy steps)
├── scripts/
│   ├── scraping_otomoto_skip_duplicates_V1.py   # Main scraping logic
│   ├── merge_csvs.py                            # Merge raw data files
│   ├── split_data_for_postgres.py               # Cleaning + final CSVs for SQL
│   ├── append_data.py                           # Insert into PostgreSQL
│   ├── init_db.py                               # DB schema + views
│   ├── makes_and_models_scraping.py             # Otomoto brands & models
│   └── utils/
│       ├── data_cleaning_utils.py               # Specialized cleaning functions
│       └── equipment_utils.py                   # Equipment parsing utilities
├── .gitignore
├── README.md
└── requirements.txt
```

---

## 🧪 Technologies Used

* Python 3.10+
* `requests`, `BeautifulSoup` (web scraping)
* `pandas`, `numpy` (data processing)
* `SQLAlchemy`, `psycopg2` (PostgreSQL connection)
* `selenium` (dynamic scraping for brand/model data)
* PostgreSQL 13+

---

## 🧹 Data Flow Overview

1. **Scraping Otomoto:**

   * Script: `scraping_otomoto_skip_duplicates_V1.py`
   * Output: `data/all_offers_otomoto_no_vin_*.csv`
   * Avoids duplicates using tracked `advert_id`s

2. **Merging and Cleaning:**

   * `merge_csvs.py` → merges raw CSVs
   * `split_data_for_postgres.py` → cleans fields, renames columns, builds equipment mapping
   * Outputs: `listings.csv`, `equipment_options.csv`, `listing_equipment.csv`

3. **PostgreSQL Storage:**

   * `init_db.py` creates schema
   * `append_data.py` loads clean data into the database, remapping `local_id` to real primary keys

---

## ⚙️ How to Run

> Project is under active development. Environment setup will be simplified soon.

### 1. Set up a virtual environment

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure PostgreSQL (local)

Make sure your PostgreSQL server is running and update the connection string if needed.

### 3. Run pipeline steps manually:

```bash
python scripts/scraping_otomoto_skip_duplicates_V1.py
python scripts/merge_csvs.py
python scripts/split_data_for_postgres.py
python scripts/init_db.py
python scripts/append_data.py
```

> You can use Jupyter Notebooks for data exploration under `notebooks/`.

---

## 📌 Plans & Roadmap

### 🔧 Short-term goals (next steps)
- [ ] Refactor scraping script to capture all relevant columns
- [ ] Improve error handling and logging in scraper
- [ ] Standardize and validate all units and column names
- [ ] Create interactive data exploration dashboard (Streamlit or Dash)

### 📊 Mid-term goals
- [ ] Support additional data sources
- [ ] Design PostgreSQL views for easier querying
- [ ] Add unit tests for cleaning and merging logic

### 🧠 Long-term / AI-focused goals
- [ ] Build ML model for price prediction based on vehicle attributes
- [ ] Implement classification of listings (e.g., by condition, seller type)
- [ ] Add car recommendation engine (based on user preferences)
- [ ] Integrate chatbot (LLM-based) to query listings or explain trends

### ⚙️ Deployment / UX ideas
- [ ] Expose API endpoints for filtered car search
- [ ] Package scraping and cleaning as CLI tool or service
- [ ] Containerize full pipeline using Docker


---

## 📂 Datasets

* Raw data is ignored in `.gitignore` due to size (>100 MB per file)
* Example mappings (`brands_and_models.csv`, `equipment_options.csv`) included
* To run full pipeline, scrape your own data or request access to anonymized sample