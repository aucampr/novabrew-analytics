# ☕ NovaBrew Analytics

A end-to-end digital marketing analytics project built on a fictional D2C coffee brand — showcasing data engineering and data analysis skills across a 10-part blog/YouTube series.

**Stack:** Python · BigQuery · dbt · Looker Studio · Jupyter

---

## What This Project Covers

NovaBrew Coffee is an 18-month-old direct-to-consumer coffee brand that hired a digital agency to build their analytics infrastructure from scratch. This repo follows that engagement across 10 posts — from raw data generation all the way to executive dashboards.

| Post | Title | Concepts | DE/DA Skills |
|------|-------|----------|--------------|
| 1 | Building the Data Foundation | Marketing funnel, KPIs, UTM tracking | Schema design, Python data generation, BigQuery |
| 2 | Customer Segmentation | RFM analysis, cohort basics, LTV | dbt staging models, SQL window functions, Jupyter EDA |
| 3 | Paid Media Teardown | ROAS, CPA, CPC, impression share | dbt marts, multi-source ingestion, Looker Studio |
| 4 | The SEO Opportunity | Keyword rankings, CTR curves, content gaps | Python analysis, pandas, matplotlib |
| 5 | Email Marketing Deep Dive | Open rate, click rate, list health, flows | Dagster pipeline, dbt snapshots, incremental loads |
| 6 | Attribution Modelling | Last-click vs linear vs time-decay | Advanced SQL, Python modelling, window functions |
| 7 | Creative Performance | Hook rate, A/B testing, creative fatigue | Statistical significance testing (scipy) |
| 8 | Black Friday Forecast | Campaign planning, scenario modelling | Prophet forecasting, pandas scenario model |
| 9 | Black Friday Results | Post-mortem, variance analysis | dbt tests, SQL variance analysis, Looker Studio |
| 10 | The 6-Month Agency Report | Executive reporting, channel mix, recommendations | dbt project maturity, full pipeline docs |

---

## Project Structure

```
novabrew-analytics/
├── data_generation/        # Synthetic data generator (Post 1)
├── dbt/                    # All SQL transformations (Posts 2–10)
│   ├── models/
│   │   ├── staging/        # Silver layer — clean raw data
│   │   ├── intermediate/   # Shared business logic
│   │   └── marts/          # Gold layer — analyst-facing tables
│   ├── tests/              # Custom data quality tests
│   └── macros/             # Reusable Jinja SQL
├── notebooks/              # Jupyter analysis (Posts 2, 4, 6, 7, 8)
├── analysis/               # Standalone Python scripts and charts
├── dashboards/             # Looker Studio screenshots + config notes
└── blog/                   # Blog post Word documents
```

---

## Quickstart

### 1. Clone the repo

```bash
git clone https://github.com/[your-handle]/novabrew-analytics.git
cd novabrew-analytics
```

### 2. Set up Python environment

```bash
python -m venv .venv
source .venv/bin/activate        # Mac/Linux
# .venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

### 3. Generate the synthetic data

```bash
cd data_generation
python generate_data.py
```

This creates 6 CSV files in `data_generation/output/`:

| File | Rows | Description |
|------|------|-------------|
| `customers.csv` | 8,000 | Customer master records |
| `orders.csv` | ~9,000 | Order headers |
| `order_items.csv` | ~15,000 | Order line items |
| `sessions.csv` | ~330,000 | Website sessions |
| `ad_spend.csv` | ~3,800 | Daily campaign spend (Google + Meta) |
| `email_events.csv` | ~85,000 | Email send/open/click/convert events |

### 4. Set up BigQuery

You'll need a Google Cloud project with BigQuery enabled (the free tier is sufficient).

```bash
# Authenticate
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

# Create dataset and load data
cd data_generation
chmod +x setup_bigquery.sh
./setup_bigquery.sh YOUR_PROJECT_ID
```

### 5. Set up dbt

```bash
cd dbt

# Copy the example profile and fill in your project details
cp profiles.yml.example profiles.yml

# Install dbt dependencies
dbt deps

# Test the connection
dbt debug

# Run the models
dbt run

# Run data quality tests
dbt test
```

---

## Data Model

```
novabrew_raw          ← Raw CSVs loaded from data_generation/output/
    ↓ dbt staging
novabrew_staging      ← Cleaned, typed, renamed
    ↓ dbt intermediate
novabrew_intermediate ← Business logic, joins
    ↓ dbt marts
novabrew_marts        ← Analyst-facing, Looker Studio connects here
```

### Key Metrics Generated

| Metric | Value |
|--------|-------|
| Period | Jan 2023 – Jun 2024 (18 months) |
| Total Revenue | ~$542,000 |
| Total Ad Spend | ~$163,000 |
| Blended ROAS | 3.33x |
| Avg Order Value | $61.29 |
| Conversion Rate | 2.73% |
| Customers | 8,000 |

---

## BigQuery Dataset Structure

Once set up, you'll have the following datasets in BigQuery:

```
your-project/
├── novabrew_raw/           # Bronze — raw ingested data
├── novabrew_staging/       # Silver — cleaned by dbt
├── novabrew_intermediate/  # Business logic layer
└── novabrew_marts/         # Gold — dashboards connect here
```

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Python | 3.9+ | [python.org](https://python.org) |
| Google Cloud SDK | Latest | [cloud.google.com/sdk](https://cloud.google.com/sdk/docs/install) |
| dbt Core + BigQuery adapter | 1.7+ | `pip install dbt-bigquery` |
| Dagster | 1.6+ | `pip install dagster dagster-webserver` |

---

## VSCode Extensions

| Extension | Purpose | When You Need It |
|-----------|---------|-----------------|
| Python (Microsoft) | Run `.py` files, IntelliSense | Post 1 onwards |
| Jupyter (Microsoft) | Run `.ipynb` notebooks inline | Posts 2, 4, 6, 7, 8 |
| BigQuery (Google Cloud) | Query BQ from VSCode | Post 1 onwards |
| dbt Power User | Run dbt models, lineage graph | Post 2 onwards |
| Rainbow CSV | Preview CSV files with coloured columns | Post 1 |
| GitLens | Git history and blame annotations | Throughout |
| Shell Script (Remisa) | Syntax highlighting for `.sh` files | Post 1 |

---

## Blog & Video Series

Posts are published at: **[your blog URL]**

YouTube playlist: **[your YouTube URL]**

LinkedIn: **[your LinkedIn URL]**

---

## License

MIT — feel free to fork and adapt for your own portfolio.

---

*All data in this project is synthetically generated for educational purposes. NovaBrew Coffee is a fictional brand.*
