# 🛒 E-Commerce Analytics & Demand Forecasting Platform

[![CI/CD](https://github.com/YOUR_USERNAME/ecommerce-data-platform/actions/workflows/deploy.yml/badge.svg)](https://github.com/YOUR_USERNAME/ecommerce-data-platform/actions)
[![Python](https://img.shields.io/badge/Python-3.10-blue.svg)](https://python.org)
[![Databricks](https://img.shields.io/badge/Databricks-Data%20Engineering-orange)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Medallion%20Architecture-blue)](https://delta.io)
[![MLflow](https://img.shields.io/badge/MLflow-Experiment%20Tracking-brightgreen)](https://mlflow.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> A **production-grade data engineering platform** built on Databricks that ingests real e-commerce data from REST APIs, processes it through a Medallion Architecture (Bronze → Silver → Gold), enforces data quality, powers SQL dashboards, and feeds an ML demand forecasting model — fully automated with CI/CD.

---

## 📋 Table of Contents
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Features](#-features)
- [Quick Start](#-quick-start)
- [Running the Pipeline](#-running-the-pipeline)
- [Data Quality](#-data-quality)
- [ML Pipeline](#-ml-pipeline)
- [CI/CD](#-cicd)
- [Databricks Setup](#-databricks-setup)

---

## 🏛️ Architecture

```
┌──────────────────────────────────────────────────────┐
│                  EXTERNAL DATA SOURCES               │
│  🛒 FakeStore API (products, orders, users)          │
│  🌦️  OpenWeatherMap API (weather features for ML)    │
└─────────────────────────┬────────────────────────────┘
                          │  Python REST API calls
                          │  Retry logic + error handling
                          ▼
┌──────────────────────────────────────────────────────┐
│           AZURE DATA LAKE STORAGE GEN2               │
│           (Bronze Landing Zone — Raw JSON)           │
│  /bronze/products/YYYY-MM-DD/                        │
│  /bronze/orders/YYYY-MM-DD/                          │
│  /bronze/weather/YYYY-MM-DD/                         │
└─────────────────────────┬────────────────────────────┘
                          │  Auto Loader (incremental)
                          ▼
┌──────────────────────────────────────────────────────┐
│                 DATABRICKS PLATFORM                  │
│                                                      │
│  🥉 BRONZE      Raw Delta tables (schema inferred)   │
│       ↓          Auto Loader + cloudFiles            │
│  🥈 SILVER      Cleaned, typed, validated            │
│       ↓          DLT Expectations (data quality)     │
│  🥇 GOLD        Business aggregations + ML features  │
│                                                      │
│  ✅ Delta Live Tables    ✅ MLflow Tracking           │
│  ✅ Great Expectations   ✅ Model Registry            │
│  ✅ Databricks SQL       ✅ Workflows Orchestration   │
└─────────────────────────┬────────────────────────────┘
                          │
┌─────────────────────────▼────────────────────────────┐
│              GITHUB ACTIONS CI/CD                    │
│   lint → unit tests → integration tests → deploy    │
└──────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Category | Technology |
|---|---|
| **Data Platform** | Databricks, Delta Lake, Delta Live Tables |
| **Orchestration** | Databricks Workflows (equivalent to Apache Airflow) |
| **Cloud Storage** | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| **Data Quality** | DLT Expectations, Great Expectations |
| **ML Tracking** | MLflow (Tracking + Model Registry) |
| **SQL Analytics** | Databricks SQL, SQL Warehouses |
| **CI/CD** | GitHub Actions, Databricks Asset Bundles |
| **Language** | Python (PySpark, pandas, scikit-learn) |
| **Testing** | pytest, unittest.mock |
| **Data Sources** | FakeStore API, OpenWeatherMap API |

---

## 📂 Project Structure

```
ecommerce-data-platform/
│
├── 📁 ingestion/
│   ├── fetch_products.py      # Product API → Bronze (with retry logic)
│   ├── fetch_orders.py        # Orders/carts API → Bronze (with flattening)
│   └── fetch_weather.py       # Weather API → Bronze (ML feature source)
│
├── 📁 dlt/
│   └── pipeline.py            # Delta Live Tables: Bronze → Silver → Gold
│                              # Includes DLT Expectations for data quality
│
├── 📁 quality/
│   └── dq_checks.py           # Great Expectations batch validation
│                              # Logs quality scores to Delta monitoring table
│
├── 📁 ml/
│   ├── feature_engineering.py # Builds ML feature table from Gold layer
│   └── demand_forecast.py     # Trains model + full MLflow tracking
│
├── 📁 sql/
│   └── dashboards.sql         # Databricks SQL dashboard queries (8 charts)
│
├── 📁 tests/
│   └── test_pipeline.py       # pytest unit tests (15+ test cases)
│
├── 📁 .github/workflows/
│   └── deploy.yml             # GitHub Actions: 4-job CI/CD pipeline
│
├── databricks.yml             # Databricks Asset Bundle (7-task Workflow)
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

---

## ✨ Features

### 🔌 API Ingestion Layer
- Consumes **FakeStore API** (products, orders, users) and **OpenWeatherMap API**
- **Exponential backoff retry logic** — handles transient API failures gracefully
- **Metadata enrichment** — adds `_ingestion_timestamp`, `_source`, `_run_date` to every record
- **Data flattening** — transforms nested cart/product JSON to flat line-item records

### 🏗️ Medallion Architecture (Bronze → Silver → Gold)
| Layer | Purpose | Key Transformations |
|---|---|---|
| 🥉 Bronze | Raw data as-is | Auto Loader, schema inference |
| 🥈 Silver | Cleaned & validated | Casting, deduplication, DLT Expectations |
| 🥇 Gold | Business-ready | Aggregations, joins, feature engineering |

### ✅ Two-Layer Data Quality
1. **DLT Expectations** (real-time) — drop/warn bad rows during pipeline execution
2. **Great Expectations** (batch) — 10+ rules per table, logs quality score to Delta

### 🤖 MLflow Demand Forecasting
- Builds feature table from Gold layer (category aggregations + weather features)
- Trains **3 model types**: Random Forest, Gradient Boosting, Ridge Regression
- Logs **all parameters, metrics, and artifacts** to MLflow
- Registers best model to **MLflow Model Registry**
- Feature importance analysis logged as artifact

### 📊 Databricks SQL Dashboards (8 queries)
- Category performance overview
- Top rated products
- Price distribution analysis
- Customer order activity
- Demand forecasting scores
- **Data quality trend monitoring**
- Inventory gap analysis (never-ordered products)
- Delta table optimization commands

### ⚙️ Databricks Workflows
- **7-task pipeline** with explicit dependency chain
- Scheduled daily at 2:00 AM
- Email alerts on failure
- Tasks: ingest → DLT → data quality → features → ML training

### 🚀 CI/CD with GitHub Actions
- **4 jobs**: lint → unit tests → integration tests → deploy
- Runs on every push to `main` and `develop`
- Deploys to Databricks workspace only on `main` branch merge
- Coverage reporting via codecov

---

## ⚡ Quick Start (Local Development)

```bash
# 1. Clone the repository
git clone https://github.com/YOUR_USERNAME/ecommerce-data-platform.git
cd ecommerce-data-platform

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run ingestion (fetches real data from FakeStore API)
python ingestion/fetch_products.py
python ingestion/fetch_orders.py
python ingestion/fetch_weather.py   # Optional: set OPENWEATHER_API_KEY env var

# 4. Run data quality checks
python quality/dq_checks.py

# 5. Build features
python ml/feature_engineering.py

# 6. Train model with MLflow tracking
python ml/demand_forecast.py

# 7. View MLflow experiments UI
mlflow ui
# Open http://localhost:5000
```

---

## 🔄 Running the Pipeline

### Local (development)
```bash
# Run full pipeline end-to-end
python ingestion/fetch_products.py && \
python ingestion/fetch_orders.py && \
python quality/dq_checks.py && \
python ml/feature_engineering.py && \
python ml/demand_forecast.py
```

### On Databricks
1. Import notebooks to `/ecommerce-platform/` in your workspace
2. Create a Delta Live Tables pipeline pointing to `dlt/pipeline.py`
3. Import `databricks.yml` and set `YOUR_DLT_PIPELINE_ID_HERE`
4. Start the Workflow from the Jobs UI or trigger via API

---

## ✅ Data Quality

Quality is enforced at **two stages**:

### Stage 1 — DLT Expectations (inline, real-time)
```python
@dlt.expect_or_drop("valid_price", "price > 0")
@dlt.expect_or_drop("title_not_empty", "title IS NOT NULL")
@dlt.expect_or_warn("valid_rating", "rating BETWEEN 0 AND 5")
```

### Stage 2 — Great Expectations (batch, after pipeline)
```
Checks per table:
  ✅ Column existence (5 checks)
  ✅ Null validation (4 checks)
  ✅ Uniqueness (1 check)
  ✅ Range validation (3 checks)
  ✅ Categorical validation (1 check)
  ✅ Type checks (2 checks)

Quality Score logged to: dq_monitoring_log Delta table
Alert threshold: < 95% = WARNING, < 80% = CRITICAL
```

---

## 🤖 ML Pipeline

```
Gold Layer Features → Feature Engineering → MLflow Experiment
                                                  │
                          ┌───────────────────────┼──────────────────────┐
                          ▼                       ▼                      ▼
                   Random Forest          Gradient Boosting           Ridge
                   (n=100, d=6)           (n=100, lr=0.1)          (alpha=1.0)
                          │                       │                      │
                          └───────────────────────┼──────────────────────┘
                                                  ▼
                                        MLflow Model Registry
                                     EcommerceDemandForecastModel
                                        (Staging → Production)
```

**Features used:**
- `product_count`, `avg_price`, `max_price`, `avg_rating`
- `price_bucket` (ordinal encoded)
- `season_encoded` (0=Winter, 1=Spring, 2=Summer, 3=Fall)
- `avg_temperature_c`, `avg_humidity_pct` (from weather API)

---

## 🚀 CI/CD

```
Push to main branch
        │
        ▼
┌──────────────────┐
│  Job 1: Lint     │ flake8, black, isort
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Job 2: Tests    │ pytest (15+ tests), coverage report
└────────┬─────────┘
         │
         ▼
┌──────────────────────┐
│  Job 3: Integration  │ Full pipeline run (real API calls)
└────────┬─────────────┘
         │
         ▼
┌──────────────────────┐
│  Job 4: Deploy       │ Databricks Asset Bundles → PROD
└──────────────────────┘
```

---

## ☁️ Databricks Setup

### Prerequisites
- Databricks workspace (Community Edition works for development)
- Azure Data Lake Storage Gen2 (or use DBFS for free)
- OpenWeatherMap API key (free at openweathermap.org)

### Secrets (for GitHub Actions CI/CD)
```
DATABRICKS_HOST   = https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN  = dapi...
OPENWEATHER_API_KEY = your_key_here
```

### DLT Pipeline Configuration
- Runtime: Databricks Runtime 13.3 LTS
- Pipeline mode: Triggered (for daily batch) or Continuous (for streaming)
- Storage: ADLS Gen2 or DBFS

---

## 📊 Sample Output

### Feature Table
```
category          product_count  avg_price  avg_rating  demand_score
electronics               6     320.75        4.35         0.87
women's clothing          4      44.99        3.98         0.64
jewelery                  3     195.50        3.70         0.42
men's clothing            3      82.65        4.15         0.58
```

### Data Quality Report
```
📊 Quality Score: 100.0% (8/8 checks passed)
  ✅ no_nulls_product_id: 0 nulls found
  ✅ no_nulls_title: 0 nulls found
  ✅ no_nulls_price: 0 nulls found
  ✅ no_nulls_category: 0 nulls found
  ✅ unique_product_id: 0 duplicates found
  ✅ positive_price: 0 rows with price <= 0
  ✅ valid_rating_range: 0 rows out of 0-5 range
```

---

## 🏆 Certification

This project was built by a **Databricks Certified Data Engineer Associate** and covers the core exam curriculum:
- ✅ Lakehouse Platform architecture
- ✅ ELT with Spark SQL and Python
- ✅ Incremental processing (Auto Loader, Structured Streaming)
- ✅ Delta Live Tables
- ✅ Workflow orchestration
- ✅ Data quality and governance

---

## 📝 License

MIT License — see [LICENSE](LICENSE) file.

---

*Built with ❤️ as a real-world Databricks Data Engineering portfolio project.*
